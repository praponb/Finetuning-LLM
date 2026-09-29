from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("country").orderBy("rating", "id")
w5 = Window.orderBy(F.col("rating").desc_nulls_first())

metrics = (
    df_suppliers
    .filter(F.col("supplier_name").like("%EuroGood%"))
    .select(
        "*",
        F.min("rating").over(w).alias("city_min_rating"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("rating").over(w3).alias("rating_avg_6"),
        F.ntile(5).over(w_ordered).alias("rating_bucket"),
        F.percent_rank().over(w5).alias("rating_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "city",
        "rating",
        "city_min_rating",
        "seq_no",
        "rating_avg_6",
        "rating_bucket",
        "rating_percent_rank",
    )
    .orderBy(F.col("rating").asc_nulls_first())
)
