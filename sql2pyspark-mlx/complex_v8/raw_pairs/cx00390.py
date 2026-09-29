from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(7, 10))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

filtered = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()) & (~F.col("c.status").isin("PAID", "OPEN")), "left_anti")
    .filter(F.col("adjuster_id") >= 5)
)

with_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.lower(F.col("a.adjuster_name")).like("%a %")), "inner")
    .filter(F.col("a.team").isNotNull())
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

windowed = (
    with_details_adjuster
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "adjuster_team",
        F.max("approved_amount").over(w_adjuster_id_by_approved_amou).alias("approved_amount_max_2"),
        F.max("approved_amount").over(w_adjuster_id_by_approved_amou_2).alias("max_approved_amount_so_far"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 1) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("F%")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "max_approved_amount_so_far",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount", "max_approved_amount_so_far"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
