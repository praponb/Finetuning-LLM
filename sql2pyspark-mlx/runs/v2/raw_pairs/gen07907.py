from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.length("supplier_name") > 9))
    .select("id", "supplier_name", F.col("rating").cast("int").alias("rating_int"))
)
