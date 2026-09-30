from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-06-28").cast("date")) & (F.col("cost") >= 210))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.datediff(F.current_date(), "launch_date") / 365).alias("years_since_launch_date"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "brand",
        F.col("years_since_launch_date").alias("YEARS_SINCE_LAUNCH_DATE_VAL"),
        "cost",
    )
    .unpivot(
        ids=["id", "brand"],
        values=["YEARS_SINCE_LAUNCH_DATE_VAL", "cost"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
