from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy(F.year("starts_on")).orderBy("starts_on_plus_months", "id")
w3 = Window.partitionBy(F.month("starts_on")).orderBy(F.col("starts_on_plus_months").desc(), "id")
w4 = Window.partitionBy(F.year("starts_on")).orderBy(F.col("starts_on_plus_months").desc(), "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "starts_on")
)

base_promotions = (
    ranked
    .filter(F.col("starts_on") < "2024-02-01")
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 3))
)

scored = (
    base_promotions
    .filter(F.year("starts_on_plus_months") == 2023)
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

result = (
    scored
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "starts_on_plus_months"))
    .groupingSets(
        [["year_start", "quarter_start"], ["year_start"]],
        "year_start",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.sum("event_seq_2").alias("total_event_seq_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
