from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")

metrics = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.ntile(4).over(w).alias("rating_bucket"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.sum("rating").over(w2).alias("running_rating_2"),
        F.count("rating").over(w3).alias("city_count_rating"),
        F.last("city", ignorenulls=True).over(w2).alias("last_known_city"),
    )
)

result = (
    metrics
    .groupBy("country")
    .agg(
        F.max("running_rating_2").alias("max_running_rating_2"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
