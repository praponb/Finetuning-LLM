from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("region") != "East")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.stddev_samp("sqft").over(w).alias("city_stddev_samp_sqft"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
    )
)
