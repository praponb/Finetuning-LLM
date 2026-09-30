from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-04-01", "2024-12-11") & ((F.col("delivered_date") > "2024-12-01") | F.col("status").isin(["IN_TRANSIT", "LOST", "RETURNED"])))
    .select("id", "cost", "status", "shipped_date", F.ceil("cost").alias("cost_whole"))
    .orderBy("status")
)
