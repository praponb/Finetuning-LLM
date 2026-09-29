from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.lead("rating").over(w).alias("next_rating"),
        F.lag("rating").over(w).alias("prev_rating"),
        F.row_number().over(w).alias("row_seq"),
    )
)
