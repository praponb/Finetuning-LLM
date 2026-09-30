from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select("id", "product_name", "category")
    .unionByName(
        df_products
        .filter(F.col("cost").isNull())
        .select("id", "product_name", "category")
    )
    .distinct()
    .orderBy("id")
)
