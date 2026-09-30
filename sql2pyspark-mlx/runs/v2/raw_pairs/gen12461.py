from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("rating") >= 3.0) & F.col("rating").between(3.5, 3.6)))
    .select("id", "supplier_name", F.floor("rating").alias("rating_whole"))
)
