from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .select("id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("id", "carrier")
    )
    .distinct()
)
