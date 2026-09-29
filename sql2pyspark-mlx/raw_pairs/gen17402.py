from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.orderBy(F.col("rating").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat_ws("-", "supplier_name", "city").alias("supplier_name_city_key"),
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)

result = (
    base
    .select(
        "id",
        "country",
        "city",
        "rating",
        "country_supplier_name_key",
        F.avg("rating").over(w_hist).alias("rating_avg_2"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.ntile(3).over(w3).alias("rating_bucket"),
    )
)
