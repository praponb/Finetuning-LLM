from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "running_sqft",
        "sqft_avg_6",
        F.when(F.col("city").rlike("^(Bangkok|Tokyo)$"), "PRIORITY").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "CORE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("sqft_avg_6").desc())
)
