from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "RETURNED", "LOST"]))
    .select("delivered_date", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("cost") > 91)
        .select("delivered_date", "shipped_date")
    )
    .distinct()
    .orderBy(F.col("shipped_date").desc())
)
