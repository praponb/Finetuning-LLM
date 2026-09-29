from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 6700)
    .select(
        "id",
        "segment",
        "city",
        "age",
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "Core").otherwise("Other").alias("segment_group"),
        F.concat_ws("/", "country", "city").alias("country_city_key"),
    )
)
