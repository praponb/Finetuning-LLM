from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "North")
    .select(
        "id",
        "sqft",
        "opened_date",
        (F.datediff(F.current_date(), "opened_date") / 30).alias("months_since_opened_date"),
        F.round("sqft", 2).alias("sqft_rounded"),
    )
)
