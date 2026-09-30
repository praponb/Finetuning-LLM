from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("p.cost")), 2).alias("avg_cost"),
        F.count(F.when(F.col("p.brand") == "Initech", 1)).alias("initech_count"),
    )
    .orderBy("avg_cost")
)
