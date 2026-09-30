from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
w3 = Window.orderBy("cost", "id")
w_ordered = Window.orderBy(F.col("cost").desc(), "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-09-04").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "carrier", "cost")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "cost")
)

metrics = (
    ranked
    .filter(F.col("cost").between(47, 83))
    .select(
        "id",
        "cost",
        F.lead("cost", 2).over(w3).alias("next_cost"),
        F.ntile(10).over(w_ordered).alias("cost_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

valid_shipments = (
    metrics
    .filter(F.col("cost_bucket") < 5)
    .select(
        "id",
        "cost",
        "cost_bucket",
        "seq_no",
        F.least("seq_no", "cost").alias("least_seq_no_cost"),
        F.greatest("cost", "cost_bucket").alias("greatest_cost_cost_bucket"),
    )
)

summary = (
    valid_shipments
    .groupBy(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("greatest_cost_cost_bucket", [0.25, 0.5, 0.75]).alias("greatest_cost_cost_bucket_quartiles"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .select("seq_no_tier", "greatest_cost_cost_bucket_quartiles", "row_count")
    .orderBy("seq_no_tier", "row_count")
)
