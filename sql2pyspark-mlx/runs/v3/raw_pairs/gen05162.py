from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 5440) | ~F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "opened_date",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy("opened_date")
)
