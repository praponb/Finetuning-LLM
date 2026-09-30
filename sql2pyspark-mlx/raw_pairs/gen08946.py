from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("cost").between(36, 92))
        .select("id", "shipped_date")
    )
    .distinct()
)
