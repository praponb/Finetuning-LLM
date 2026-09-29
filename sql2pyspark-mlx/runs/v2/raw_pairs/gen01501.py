from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(((F.col("delivered_date") >= "2023-12-26") & F.col("cost").between(43, 91)) | (F.col("delivered_date") < "2025-03-28")))
    .select("cost", F.col("cost").cast("int").alias("cost_int"))
)
