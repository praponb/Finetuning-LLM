from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Home", "Books"]))
    .groupBy("brand")
    .agg(
        F.max("cost").alias("max_cost"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
    .filter(F.col("max_cost") >= 440)
    .select("brand", "max_cost", "unique_suppliers")
    .orderBy(F.col("unique_suppliers").desc())
)
