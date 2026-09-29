from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("price") < 870) & (F.col("supplier_id").isNotNull() | F.col("category").isin(["Grocery", "Books", "Sports"]))))
    .select(
        "category",
        "stock_qty",
        "launch_date",
        F.upper("brand").alias("brand_upper"),
    )
    .orderBy(F.col("brand_upper").desc())
)
