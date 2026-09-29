from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "London") & F.col("supplier_name").like("L%"))
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .orderBy("unique_countrys")
)
