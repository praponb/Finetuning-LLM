from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-09-17").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
    )
)

result = (
    cleaned
    .groupBy("category")
    .pivot("brand", ["Stark", "Acme", "Globex"])
    .agg(F.avg("cost").alias("value_sum"))
)
