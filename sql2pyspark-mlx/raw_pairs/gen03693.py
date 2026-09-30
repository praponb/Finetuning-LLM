from pyspark.sql import functions as F

long_format = (
    df_orders
    .select("id", "status", "discount", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "amount", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)

result = (
    long_format
    .groupBy(
        F.when(F.col("reading") >= 62, "HIGH").otherwise("LOW").alias("reading_tier"),
        "status",
    )
    .agg(
        F.round(F.sum("reading") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("reading_per_pending"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.avg("reading").alias("avg_reading"),
        F.min("reading").alias("min_reading"),
    )
    .filter(F.col("cancelled_rows") >= 1)
)
