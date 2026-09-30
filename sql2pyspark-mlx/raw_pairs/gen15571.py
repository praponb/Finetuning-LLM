from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("adjusted_sqft")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("opened_date", "id")

filtered = (
    df_stores
    .filter(F.col("sqft") > 11803)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.round((F.col("sqft") * 0.9), 2).alias("adjusted_sqft"),
        F.concat("city", F.lit("/"), "region").alias("city_region"),
    )
)

result = (
    filtered
    .select(
        "id",
        "region",
        "opened_date",
        "city_region",
        F.cume_dist().over(w).alias("adjusted_sqft_cume_dist"),
        F.sum("adjusted_sqft").over(w2).alias("cumulative_adjusted_sqft"),
        F.lead("adjusted_sqft", 1).over(w3).alias("next_adjusted_sqft"),
    )
)
