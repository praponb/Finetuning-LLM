from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(287, 343) & (F.col("price") >= 760) & F.col("supplier_id").isNull())
    .select(
        "supplier_id",
        "category",
        F.when(F.col("brand").isin(["Acme", "Globex"]), "Group A").otherwise("Other").alias("brand_group"),
    )
    .orderBy("brand_group", F.col("supplier_id").desc())
)
