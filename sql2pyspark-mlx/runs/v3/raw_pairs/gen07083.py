from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(560, 650) & (F.col("supplier_id") == 19) & F.col("supplier_id").isin([7, 14]))
    .agg(
        F.count("*").alias("total_count"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("total_cost")
)
