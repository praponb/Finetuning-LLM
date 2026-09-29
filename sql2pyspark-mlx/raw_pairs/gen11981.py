from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 8300)
    .select("city")
    .subtract(
        df_stores
        .filter(~F.col("region").isin(["South", "East"]))
        .select("city")
    )
)
