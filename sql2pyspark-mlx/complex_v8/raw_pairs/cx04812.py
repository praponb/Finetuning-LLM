from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("region").orderBy("joined_on", "id")
w_region_segment_by_joined_on = Window.partitionBy("region", "segment").orderBy(F.col("joined_on").desc(), "id")
w_region_segment_by_joined_on_2 = Window.partitionBy("region", "segment").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_joined_on_ties = Window.orderBy(F.col("joined_on").desc())
w_segment_region = Window.partitionBy("segment", "region")
w_all_by_joined_on_running = Window.orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_holders = (
    df_policyholders
    .filter((~F.col("segment").isin("RETAIL", "VIP")) & (F.col("risk_score").between(0.1, 0.7)))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

deduped = (
    cleaned_holders
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    deduped
    .select(
        "*",
        F.rank().over(w_region_segment_by_joined_on).alias("joined_on_rank"),
        F.first("joined_on").over(w_region_segment_by_joined_on_2).alias("first_joined_on"),
        F.round(F.cume_dist().over(w_all_by_joined_on_ties), 3).alias("joined_on_cume_dist"),
        F.sum("risk_score").over(w_all_by_joined_on_running).alias("running_risk_score"),
        F.round(F.percent_rank().over(w_all_by_joined_on_ties), 3).alias("joined_on_percent_rank"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.product").isNotNull()), "left_anti")
)

result = (
    without_matches
    .select("id", "running_risk_score", "joined_on_cume_dist")
    .unpivot(
        ids=["id"],
        values=["running_risk_score", "joined_on_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
