from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("opened_date", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-27").cast("date")) & (F.col("city") == "London"))
    .withColumn("region_title", F.initcap(F.trim("region")))
    .withColumn("store_name_lower", F.lower("store_name"))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

result = (
    valid
    .select(
        "id",
        "city",
        "opened_date",
        "region_title",
        "store_name_lower",
        F.count("sqft").over(w).alias("last4_count_sqft"),
        F.min("sqft").over(w).alias("last4_min_sqft"),
        F.avg("sqft").over(w).alias("last4_avg_sqft"),
        F.lead("sqft", 1, 0).over(w2).alias("next_sqft"),
    )
)
