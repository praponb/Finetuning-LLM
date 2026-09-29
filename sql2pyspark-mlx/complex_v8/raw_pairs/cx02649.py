from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_team_not_in_prope = Window.orderBy(F.col("pct_team_not_in_property_health").desc(), "level", "first_claim_reported_at_year", "last_claim_reported_at_month")
w_level_first_claim_reported_a = Window.partitionBy("level", "first_claim_reported_at_year")
w_all_by_pct_team_not_in_prope_2 = Window.orderBy(F.col("pct_team_not_in_property_health").desc(), "level", "first_claim_reported_at_year", "last_claim_reported_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_last_claim_reported_at = Window.partitionBy("level", F.date_trunc("month", "last_claim_reported_at_month")).orderBy("n_team_property", "level", "first_claim_reported_at_year", "last_claim_reported_at_month")

base = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

profiled = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.avg_claim_claimed_amount",
        "c.first_claim_reported_at",
    )
)

per_group = (
    profiled
    .groupBy(
        "level",
        F.year("first_claim_reported_at").alias("first_claim_reported_at_year"),
        F.date_trunc("month", "last_claim_reported_at").alias("last_claim_reported_at_month"),
    )
    .agg(
        F.count(F.when(F.col("team") == "Property", 1)).alias("n_team_property"),
        F.round(100 * F.count(F.when(~F.col("team").isin("Property", "Health"), 1)) / F.count("*"), 1).alias("pct_team_not_in_property_health"),
    )
)

sequenced = (
    per_group
    .select(
        "*",
        F.row_number().over(w_all_by_pct_team_not_in_prope).alias("seq_no"),
        F.count(F.lit(1)).over(w_level_first_claim_reported_a).alias("rows_in_level_first_claim_reported_at_year"),
        F.last("level").over(w_all_by_pct_team_not_in_prope_2).alias("last_level"),
        F.lag("first_claim_reported_at_year").over(w_level_last_claim_reported_at).alias("prev_first_claim_reported_at_year"),
    )
)

tagged = (
    sequenced
    .filter(F.col("pct_team_not_in_property_health") <= 55.0)
    .withColumn("level_filled", F.coalesce(F.col("level"), F.lit(0)))
)

annotated = (
    tagged
    .select(
        "*",
        F.when(F.col("level_filled") >= 4, "large").when(F.col("level_filled") >= 2, "medium").otherwise("small").alias("level_filled_tier"),
        (F.col("level_filled") + F.col("n_team_property")).alias("level_filled_plus_n_team_property"),
        F.abs(F.col("seq_no") - 4).alias("seq_no_distance"),
    )
)

result = (
    annotated
    .groupingSets(
        [["prev_first_claim_reported_at_year", "level_filled", "first_claim_reported_at_year"], ["prev_first_claim_reported_at_year"], ["level_filled"], []],
        "prev_first_claim_reported_at_year", "level_filled", "first_claim_reported_at_year",
    )
    .agg(
        F.sum("level").alias("total_level"),
        F.min("rows_in_level_first_claim_reported_at_year").alias("min_rows_in_level_first_claim_reported_at_year"),
    )
    .orderBy(
        F.col("prev_first_claim_reported_at_year").asc_nulls_last(),
        F.col("level_filled").asc_nulls_last(),
        F.col("first_claim_reported_at_year").asc_nulls_last(),
    )
)
