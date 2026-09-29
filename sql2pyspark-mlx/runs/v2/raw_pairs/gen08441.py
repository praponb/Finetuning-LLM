from pyspark.sql import functions as F

src = df_products.filter(F.col("cost") < 260)

category_summary = (
    src
    .groupBy("category", "supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("cost")).otherwise(0)).alias("stark_cost"),
    )
)

result = (
    category_summary
    .select("category", "supplier_id", "stark_cost")
    .orderBy(F.col("supplier_id").desc())
)
