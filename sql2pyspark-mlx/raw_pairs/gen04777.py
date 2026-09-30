from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]) & (F.col("opened_date") <= "2024-01-01") & (F.col("opened_date") >= "2024-09-01"))
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        F.when(F.col("region") == "West", 1).otherwise(0).alias("is_west"),
    )
    .orderBy("region")
)
