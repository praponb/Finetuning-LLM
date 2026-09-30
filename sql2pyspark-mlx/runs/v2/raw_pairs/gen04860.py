from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("city") == "Berlin") & (F.col("sqft") < 6299)) | F.col("city").isin(["Tokyo", "Berlin"]))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.date_sub("opened_date", 77).alias("opened_date_minus_77d"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("city").desc())
)
