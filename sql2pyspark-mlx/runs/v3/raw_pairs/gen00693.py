from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft").between(4695, 13967))
    .select(
        "id",
        "region",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
    )
)
