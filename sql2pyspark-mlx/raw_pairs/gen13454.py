from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region").orderBy("opened_date", "id")

enriched = (
    df_stores
    .withColumn("next_sqft", F.lead("sqft").over(w_running))
    .withColumn("prev_sqft", F.lag("sqft").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "next_sqft",
        "prev_sqft",
        "row_seq",
    )
    .orderBy("prev_sqft")
)
