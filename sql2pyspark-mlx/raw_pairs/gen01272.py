from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").asc_nulls_first())

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        F.lead("sqft", 2).over(w).alias("next_sqft"),
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.last("region", ignorenulls=True).over(w3).alias("last_known_region_2"),
        F.cume_dist().over(w4).alias("sqft_cume_dist"),
    )
)

result = enriched.select("id", "store_name", "next_sqft", "last_known_region_2")
