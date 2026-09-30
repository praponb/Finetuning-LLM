from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "city",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
    .orderBy("id")
    .limit(5)
)
