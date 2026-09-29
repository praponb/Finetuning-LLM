from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 10300)
    .select("id", "name")
    .intersect(
        df_customers
        .filter(~F.col("segment").isin(["Corporate", "Consumer"]))
        .select("id", "name")
    )
)
