from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "store_name",
        F.date_add("opened_date", 23).alias("opened_date_plus_23d"),
    )
)
