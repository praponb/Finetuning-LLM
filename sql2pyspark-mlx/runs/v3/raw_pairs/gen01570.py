from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("region") == "Central")
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.ntile(2).over(w).alias("sqft_ntile"),
        F.last("opened_date").over(w2).alias("last_opened_date"),
    )
)
