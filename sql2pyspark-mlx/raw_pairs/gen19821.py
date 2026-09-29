from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "DE") & ~F.col("supplier_name").like("T%"))
    .groupBy("country")
    .agg(F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"))
    .orderBy("country")
)
