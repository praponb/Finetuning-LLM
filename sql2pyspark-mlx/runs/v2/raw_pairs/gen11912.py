from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").like("L%"))
    .withColumn("city_group", F.when(F.col("city").isin(["London", "Bangkok"]), "Core").otherwise("Other"))
)

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(F.count("rating").alias("rating_count"))
)

result = country_summary
