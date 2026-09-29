from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 27)
    .select("id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") > "2025-01-29")
        .select("id", "carrier")
    )
    .distinct()
    .orderBy(F.col("carrier").desc())
)
