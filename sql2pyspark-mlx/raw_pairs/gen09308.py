from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 87) & F.col("cost").isNull() & (F.col("status") != "IN_TRANSIT"))
)
