from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isin([17, 25, 29, 36]))
        .select("country")
    )
    .distinct()
    .orderBy("country")
)
