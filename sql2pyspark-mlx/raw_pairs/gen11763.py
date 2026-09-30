from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-18").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit_2"),
    )
)

result = (
    filtered
    .select(
        "id",
        "store_id",
        F.col("revenue_per_unit").alias("REVENUE_PER_UNIT_VAL"),
        F.col("revenue_per_unit_2").alias("REVENUE_PER_UNIT_2_VAL"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["REVENUE_PER_UNIT_VAL", "REVENUE_PER_UNIT_2_VAL"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
