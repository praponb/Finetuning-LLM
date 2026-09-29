from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .select("carrier", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([7, 19, 38, 39]))
        .select("carrier", "shipped_date")
    )
    .orderBy("carrier")
)
