from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft").over(w_ordered).alias("prev_sqft"),
        F.lead("sqft").over(w_ordered).alias("next_sqft"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)
