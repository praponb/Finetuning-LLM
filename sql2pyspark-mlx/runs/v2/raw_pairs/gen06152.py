from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") <= "2023-05-23") & (F.col("delivered_date") > "2023-11-01") & F.col("shipped_date").between("2023-03-21", "2024-04-06"))
    .select("id", "status")
    .orderBy("status", "id")
    .limit(20)
)
