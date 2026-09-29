from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("city").isin(["New York", "Bangkok"]), "core").otherwise("other").alias("city_group"),
        "city",
    )
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("latest_opened_date", "city_group")
)
