from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED", "IN_TRANSIT"]) & F.col("cost").between(26, 84))
    .select("delivered_date")
)
