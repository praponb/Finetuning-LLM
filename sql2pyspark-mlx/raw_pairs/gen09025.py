from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "region")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "region",
        "city",
        "sqft",
        F.count("*").over(w).alias("opened_date_year_count"),
        F.last("sqft").over(w2).alias("last_sqft"),
    )
    .orderBy("last_sqft")
)
