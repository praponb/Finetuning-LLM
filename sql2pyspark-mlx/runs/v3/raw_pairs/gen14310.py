from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "North") & (F.col("city") == "Chiang Mai") & ~F.col("city").isin(["Chiang Mai", "Berlin", "Bangkok"]))
    .select(
        "id",
        "sqft",
        "region",
        "opened_date",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
    )
    .orderBy("region")
)
