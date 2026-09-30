from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w4 = Window.partitionBy("country", "city")

scored = (
    df_suppliers
    .filter(F.col("country") != "JP")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.row_number().over(w_running).alias("event_seq"),
        F.lag("rating", 2).over(w2).alias("prev_rating"),
        F.rank().over(w3).alias("rating_rank"),
        F.count("rating").over(w4).alias("country_count_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "country",
        "city",
        "rating",
        "event_seq",
        "rating_rank",
        "country_count_rating",
    )
    .orderBy(F.col("event_seq").desc())
)
