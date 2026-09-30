from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.11, 0.37))
    .select("status")
    .unionByName(
        df_orders
        .filter(~F.col("status").isin(["SHIPPED", "RETURNED", "PENDING"]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
