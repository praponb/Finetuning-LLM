from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-09-16").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .select("id", "category", "price", "cost")
    .unpivot(
        ids=["id", "category"],
        values=["price", "cost"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
