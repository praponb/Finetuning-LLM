from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["UK", "JP"]))
    .select("id", "country")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") > 15600)
        .select("id", "country")
    )
)
