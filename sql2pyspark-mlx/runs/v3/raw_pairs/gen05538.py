from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select("carrier")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") > "2024-04-01")
        .select("carrier")
    )
    .orderBy(F.col("carrier").desc())
)
