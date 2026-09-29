from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "supplier_id",
        F.concat("product_name", F.lit(" "), "brand").alias("product_name_brand"),
        F.when(F.col("category").isin(["Fashion", "Grocery"]), "Priority").otherwise("Other").alias("category_group"),
    )
)
