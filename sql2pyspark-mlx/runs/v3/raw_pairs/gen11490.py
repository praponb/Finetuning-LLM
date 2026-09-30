from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "email",
        F.last("email").over(w).alias("last_email"),
    )
)
