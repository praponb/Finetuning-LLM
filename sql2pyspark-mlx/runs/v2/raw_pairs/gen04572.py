from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "KANYA SRISUK")
    .select("segment")
    .subtract(
        df_customers
        .filter(F.col("country").isin(["TH", "US"]))
        .select("segment")
    )
    .orderBy(F.col("segment").desc())
)
