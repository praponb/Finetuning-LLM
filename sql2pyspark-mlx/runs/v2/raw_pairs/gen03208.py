from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(550, 630) & F.col("category").isin(["Sports", "Books"]))
    .select(
        "id",
        "launch_date",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
    )
)
