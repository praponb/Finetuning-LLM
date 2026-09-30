from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

result = (
    cleaned
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.max("sqft").over(w).alias("city_max_sqft"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
        F.sum("sqft").over(w3).alias("running_sqft"),
    )
)
