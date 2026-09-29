from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("region").isNull())
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.lead("sqft", 1).over(w).alias("next_sqft"),
    )
    .orderBy(F.col("sqft").desc(), "opened_date")
)
